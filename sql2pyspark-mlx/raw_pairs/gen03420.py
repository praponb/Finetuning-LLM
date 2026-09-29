from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id", "category").orderBy("launch_date_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("category")
w3 = Window.partitionBy("supplier_id", "category").orderBy("last_known_cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy(F.year("launch_date"), "supplier_id").orderBy("launch_date_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("category").orderBy("launch_date_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_products
    .filter(F.col("launch_date") > F.to_date(F.lit("2023-03-24")))
    .select(
        "id",
        "product_name",
        "category",
        "price",
        "cost",
        "launch_date",
        "tags",
        "supplier_id",
        F.trim("category").alias("clean_category"),
    )
)

base = (
    valid
    .filter(F.col("supplier_id").isNotNull())
    .select(
        "*",
        F.date_trunc("week", "launch_date").alias("launch_date_period"),
        F.trim("category").alias("clean_category_2"),
    )
)

metrics = (
    base
    .select(
        "id",
        "category",
        "price",
        "launch_date",
        "tags",
        "supplier_id",
        "clean_category",
        "launch_date_period",
        "clean_category_2",
        F.last("cost", ignorenulls=True).over(w).alias("last_known_cost"),
        F.corr("cost", "price").over(w2).alias("corr_cost_price"),
    )
)

scored = (
    metrics
    .withColumn("running_last_known_cost", F.sum("last_known_cost").over(w3))
    .withColumn("cumulative_last_known_cost", F.sum("last_known_cost").over(w4))
    .withColumn("first_known_price", F.first("price", ignorenulls=True).over(w5))
)

eligible = (
    scored
    .alias("s")
    .join(df_inventory.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.last_restock") <= F.col("s.launch_date")), "left_semi")
)

matched_keys = (
    eligible
    .select("id")
    .subtract(
        df_products
        .filter(F.col("stock_qty").between(194, 327))
        .select("id")
    )
)

reconciled = (
    eligible
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

remaining_keys = (
    reconciled
    .select("supplier_id", "id")
    .intersectAll(
        df_products
        .filter(~F.col("brand").isin(["Stark", "Globex", "Umbrella"]))
        .select("supplier_id", "id")
    )
)

verified = (
    reconciled
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.supplier_id") == F.col("k.supplier_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .select(
        "id",
        "price",
        "launch_date",
        "tags",
        "clean_category",
        "launch_date_period",
        "clean_category_2",
        "cumulative_last_known_cost",
    )
    .orderBy(F.col("launch_date_period").asc_nulls_first())
)
