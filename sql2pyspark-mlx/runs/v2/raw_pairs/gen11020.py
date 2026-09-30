from pyspark.sql import functions as F

valid_customers = (
    df_customers
    .filter(F.col("email").like("%@startup.io"))
    .withColumn("is_active_label", F.when(F.col("is_active"), "Yes").otherwise("No"))
)

segment_summary = (
    valid_customers
    .groupBy("segment")
    .agg(F.sum("credit_limit").alias("total_credit_limit"))
)

result = segment_summary.select("segment", "total_credit_limit")
