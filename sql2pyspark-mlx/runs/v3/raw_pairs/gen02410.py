from pyspark.sql import functions as F

src = (
    df_customers
    .filter((F.length("email") >= 14) & (F.col("city") == "Chiang Mai"))
    .withColumn("segment_upper", F.upper("segment"))
)

customers_stats = (
    src
    .groupBy("city", "country")
    .agg(
        F.min("credit_limit").alias("min_credit_limit"),
        F.sum("age").alias("total_age"),
    )
)

result = (
    customers_stats
    .select("city", "country", "min_credit_limit", "total_age")
    .orderBy("country")
)
