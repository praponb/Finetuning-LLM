from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")

valid = (
    df_store_sales
    .filter(F.col("sale_date") >= F.lit("2023-07-07").cast("date"))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        (F.datediff(F.current_date(), "sale_date") / 365).alias("years_since_sale_date"),
    )
)

remaining_keys = (
    valid
    .select("product_id", "id")
    .exceptAll(
        df_store_sales
        .filter(F.col("units") == 26)
        .select("product_id", "id")
    )
)

verified = (
    valid
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_dims = (
    verified
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "left")
    .select(
        "e.id",
        "e.store_id",
        "e.product_id",
        "e.sale_date",
        "e.units",
        "e.years_since_sale_date",
        F.col("p.category"),
        F.col("p.product_name"),
        F.col("p.cost"),
    )
)

metrics = (
    with_dims
    .filter(F.col("store_id").isin([1, 2, 3, 4]))
    .withColumn("cost_share", F.round((F.col("cost") / F.sum("cost").over(w)), 4))
)

joined = (
    metrics
    .alias("e")
    .join(df_products.alias("d"), F.col("e.product_id") == F.col("d.id"), "inner")
    .select(
        "e.id",
        "e.store_id",
        "e.product_id",
        "e.years_since_sale_date",
        "e.cost",
        "e.cost_share",
        F.col("d.cost").alias("product_cost"),
        F.col("d.stock_qty"),
    )
)

kept_keys = (
    joined
    .select("id", "product_id")
    .subtract(
        df_store_sales
        .filter(F.month("sale_date").isin([2, 10, 11]))
        .select("id", "product_id")
    )
)

reconciled = (
    joined
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select("store_id", "years_since_sale_date", "cost", "cost_share", "product_cost")
    .orderBy(F.col("store_id").desc())
)
