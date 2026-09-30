from pyspark.sql import functions as F

valid = (
    df_products
    .filter(F.col("brand").isNotNull())
    .select("*", (F.col("cost") + F.col("stock_qty")).alias("cost_plus_stock_qty"))
)

result = (
    valid
    .groupBy(
        F.date_trunc("month", "launch_date").alias("activity_month"),
        F.date_trunc("quarter", "launch_date").alias("quarter_start"),
    )
    .agg(
        F.corr("cost", "price").alias("corr_cost_price"),
        F.count(F.when(F.col("brand") == "Stark", 1)).alias("stark_count"),
    )
    .filter(F.col("stark_count") < 1000)
    .orderBy(F.col("stark_count").asc_nulls_first())
)
