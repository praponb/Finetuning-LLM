from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("revenue").desc())
w_grp = Window.partitionBy("product_id").orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_store_sales
    .select(
        "*",
        F.cume_dist().over(w).alias("revenue_cume_dist"),
        F.first("units", ignorenulls=True).over(w_grp).alias("first_known_units"),
    )
)

result = (
    enriched
    .groupBy(F.date_trunc("week", "sale_date").alias("week_start"))
    .agg(
        F.min("revenue").alias("min_revenue"),
        F.stddev_samp("revenue").alias("revenue_stddev"),
        F.count("*").alias("num_rows"),
        F.corr("units", "revenue").alias("corr_units_revenue"),
    )
    .orderBy("week_start")
)
