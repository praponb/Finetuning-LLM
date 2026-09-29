from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region", "city").orderBy("sqft")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.units").desc(), F.col("c.id").desc())
w3 = Window.orderBy("store_sale_revenue", "id")
w_recent = Window.orderBy("store_sale_revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_stores
    .filter(F.col("region").isNull())
    .withColumn("years_since_opened_date", (F.datediff(F.current_date(), "opened_date") / 365.25))
    .withColumn("city_prefix", F.substring("city", 1, 4))
)

ranked = (
    cleaned
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
)

with_latest = (
    ranked
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.years_since_opened_date",
        "s.city_prefix",
        F.col("c.sale_date").alias("store_sale_sale_date"),
        F.col("c.revenue").alias("store_sale_revenue"),
    )
)

kept = (
    with_latest
    .alias("s")
    .join(df_inventory.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.last_restock") < F.col("s.store_sale_sale_date")), "left_semi")
)

enriched = (
    kept
    .filter(F.col("store_sale_sale_date") >= "2023-12-01")
    .select(
        "id",
        "years_since_opened_date",
        "store_sale_revenue",
        F.ntile(5).over(w3).alias("store_sale_revenue_bucket"),
        F.last("store_sale_revenue", ignorenulls=True).over(w_recent).alias("last_known_store_sale_revenue"),
    )
)

result = (
    enriched
    .rollup(
        F.when(F.col("years_since_opened_date") >= 72, "HIGH").otherwise("LOW").alias("years_since_opened_date_tier"),
        F.when(F.col("last_known_store_sale_revenue") >= 4200, "HIGH").otherwise("LOW").alias("last_known_store_sale_revenue_tier"),
    )
    .agg(
        F.sum("years_since_opened_date").alias("total_years_since_opened_date"),
        F.round(F.avg("years_since_opened_date"), 2).alias("avg_years_since_opened_date"),
        F.percentile_approx("last_known_store_sale_revenue", [0.25, 0.5, 0.75]).alias("last_known_store_sale_revenue_quartiles"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("years_since_opened_date"), 2).alias("avg_years_since_opened_date_2"),
    )
    .filter(F.col("avg_years_since_opened_date_2") > 0)
    .orderBy(F.col("years_since_opened_date_tier").desc())
)
