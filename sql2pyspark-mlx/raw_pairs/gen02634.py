from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .filter(F.lower(F.trim(F.col("s.store_name"))) == "downtown express")
    .groupBy(F.col("s.region"))
    .agg(
        F.sum(F.col("ss.revenue")).alias("total_revenue"),
        F.min(F.col("ss.sale_date")).alias("first_sale_date"),
    )
    .orderBy(F.col("first_sale_date").desc(), "region")
    .limit(20)
)
