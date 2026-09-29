from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("age") != 59) & (F.col("country") != "TH") & (F.col("segment") == "Corporate"))
    .groupBy("country")
    .agg(
        F.max("credit_limit").alias("max_credit_limit"),
        F.countDistinct("credit_limit").alias("unique_segments"),
    )
    .orderBy(F.col("max_credit_limit").desc(), F.col("unique_segments").desc())
)
