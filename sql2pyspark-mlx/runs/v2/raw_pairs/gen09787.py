from pyspark.sql import functions as F

recent_store_sales = (
    df_store_sales
    .filter(F.month("sale_date").isin([6, 7, 10]) & F.col("units").between(38, 39))
    .withColumn("sale_date_plus_44d", F.date_add("sale_date", 44))
)

store_id_summary = (
    recent_store_sales
    .groupBy("store_id", F.month("sale_date_plus_44d").alias("sale_plus_44d_month"))
    .agg(
        F.max("sale_date_plus_44d").alias("latest_sale_date_plus_44d"),
        (F.max("revenue") - F.min("revenue")).alias("revenue_range"),
    )
)

result = (
    store_id_summary
    .select(
        "store_id",
        "sale_plus_44d_month",
        "latest_sale_date_plus_44d",
        "revenue_range",
    )
    .orderBy("store_id", "sale_plus_44d_month")
    .limit(1)
)
