from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("age").between(47, 61))
    .groupBy(
        F.when(F.col("segment").isin(["Consumer", "SMB"]), "group_a").otherwise("other").alias("segment_group"),
        F.month("signup_date").alias("signup_month"),
    )
    .agg(
        F.avg("credit_limit").alias("avg_credit_limit"),
        F.count("*").alias("total_count"),
    )
    .orderBy("segment_group", F.col("avg_credit_limit").desc())
)
