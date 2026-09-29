from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy(F.col("weeks_since_sale_date").desc_nulls_last())
w2 = Window.partitionBy("store_id").orderBy(F.col("revenue").desc())
w3 = Window.partitionBy("store_id", "product_id")
w4 = Window.partitionBy(F.date_trunc("month", "sale_date"), "product_id")

cleaned = (
    df_store_sales
    .filter(F.col("units").between(23, 40))
    .withColumn("weeks_since_sale_date", (F.datediff(F.current_date(), "sale_date") / 7))
)

matched_keys = (
    cleaned
    .select("id")
    .intersect(
        df_store_sales
        .filter(F.col("revenue") < 1900)
        .select("id")
    )
)

kept_rows = (
    cleaned
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = (
    kept_rows
    .select(
        "*",
        F.percent_rank().over(w).alias("weeks_since_sale_date_percent_rank"),
        F.cume_dist().over(w2).alias("revenue_cume_dist"),
        F.corr("units", "revenue").over(w3).alias("corr_units_revenue"),
        F.round((F.col("units") / F.sum("units").over(w4)), 4).alias("units_share"),
    )
)

joined = (
    enriched
    .alias("s")
    .join(df_stores.alias("p"), F.col("s.store_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.city"), F.col("p.store_name"), F.col("p.region"))
)

result = (
    joined
    .select("store_id", "corr_units_revenue", "units_share", "weeks_since_sale_date")
    .unpivot(
        ids=["store_id"],
        values=["corr_units_revenue", "units_share", "weeks_since_sale_date"],
        variableColumnName="measure",
        valueColumnName="metric_value",
    )
)
