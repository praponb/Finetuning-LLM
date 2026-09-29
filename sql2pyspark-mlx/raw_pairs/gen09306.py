from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("units_str").desc(), "id")

valid = (
    df_store_sales
    .filter(F.col("sale_date") > F.lit("2023-04-13").cast("date"))
    .select(
        "*",
        F.date_trunc("MONTH", "sale_date").alias("sale_date_period"),
        F.col("units").cast("double").alias("units_str"),
    )
)

joined = (
    valid
    .alias("s")
    .join(df_stores.alias("p"), F.col("s.store_id") == F.col("p.id"), "left")
    .filter(F.col("p.city").isin(["Bangkok", "New York"]))
    .select(
        "s.id",
        "s.store_id",
        "s.product_id",
        "s.sale_date",
        "s.units",
        "s.revenue",
        "s.units_str",
        F.col("p.region"),
    )
)

base = (
    joined
    .filter((F.col("sale_date") >= F.lit("2023-08-24").cast("date")) & F.col("region").isNotNull())
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        "units_str",
        "region",
        F.col("units_str").cast("int").alias("units_str_int"),
        F.coalesce("region", F.lit("N/A")).alias("region_filled"),
    )
)

deduped = (
    base
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    deduped
    .groupBy("store_id")
    .pivot("region", ["South", "East", "West"])
    .agg(F.avg("revenue").alias("total"))
)
