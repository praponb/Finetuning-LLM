from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("country").isin(["US", "UK", "SG"]) & (F.col("segment") == "Consumer"))
    .withColumn("name_email", F.concat("name", F.lit(" - "), "email"))
)
