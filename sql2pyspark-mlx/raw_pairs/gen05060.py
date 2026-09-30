from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("supplier_id").orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_products
    .filter((F.col("launch_date") > F.to_date(F.lit("2023-03-20"))) & (~F.col("brand").eqNullSafe(F.lit("Initech"))))
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        F.sort_array("tags").alias("sorted_tags"),
    )
)

not_flagged = (
    valid
    .alias("s")
    .join(df_inventory.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.last_restock").between("2025-01-11", "2025-08-01")), "left_anti")
)

metrics = (
    not_flagged
    .select(
        "id",
        "product_name",
        "price",
        "cost",
        "tags",
        F.max("cost").over(w_ordered).alias("running_max_cost"),
        F.min("cost").over(w_ordered).alias("running_min_cost"),
        F.sum("cost").over(w_ordered).alias("running_sum_cost"),
    )
)

eligible = (
    metrics
    .alias("s")
    .join(df_inventory.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.quantity") >= 185), "left_anti")
)

matched_keys = (
    eligible
    .select("id")
    .subtract(
        df_products
        .filter(~F.col("brand").isin(["Umbrella", "Stark", "Initech"]))
        .select("id")
    )
)

kept_rows = (
    eligible
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select(
        "id",
        "price",
        "cost",
        "running_max_cost",
        "running_min_cost",
        "running_sum_cost",
    )
    .orderBy(F.col("running_max_cost").desc_nulls_last(), "running_sum_cost")
)
