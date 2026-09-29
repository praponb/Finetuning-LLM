from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("is_active"))
    .withColumn("is_active_label", F.when(F.col("is_active"), "Yes").otherwise("No"))
)
