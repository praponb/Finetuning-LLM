from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region", "city").orderBy("opened_date", "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.sale_date").desc(), F.col("c.id").desc())
w3 = Window.orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_stores
    .filter(F.col("opened_date") > F.to_date(F.lit("2023-05-10")))
    .select(
        "*",
        F.initcap(F.trim("city")).alias("city_title"),
        F.coalesce("region", F.lit("DIRECT")).alias("region_filled"),
    )
)

enriched = (
    base
    .select(
        "id",
        "opened_date",
        "sqft",
        "region_filled",
        F.lead("sqft").over(w).alias("next_sqft"),
        F.row_number().over(w).alias("row_seq"),
    )
)

with_latest = (
    enriched
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.sale_date") <= F.col("s.opened_date")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.revenue"))
)

enriched_stores = (
    with_latest
    .filter(F.col("revenue") < 3400)
    .select(
        "*",
        F.max("next_sqft").over(w3).alias("running_max_next_sqft"),
        F.min("next_sqft").over(w3).alias("running_min_next_sqft"),
        F.sum("next_sqft").over(w3).alias("running_sum_next_sqft"),
        F.last("next_sqft", ignorenulls=True).over(w4).alias("last_known_next_sqft"),
    )
)

result = (
    enriched_stores
    .select(
        "id",
        "opened_date",
        "sqft",
        "region_filled",
        "next_sqft",
        "row_seq",
        "revenue",
        "running_max_next_sqft",
        "running_min_next_sqft",
    )
    .orderBy(F.col("revenue").desc())
)
