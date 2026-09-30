from pyspark.sql import functions as F

result = (
    df_transactions
    .withColumn("is_flagged_label", F.when(F.col("is_flagged"), "Yes").otherwise("No"))
    .withColumn("merchant_length", F.length("merchant"))
)
