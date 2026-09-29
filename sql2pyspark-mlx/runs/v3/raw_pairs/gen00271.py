from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("segment").isin(["Corporate", "SMB", "Consumer"]) & F.col("segment").isin(["SMB", "Consumer", "Corporate"]))
    .withColumn("name_city_key", F.concat_ws("-", "name", "city"))
    .orderBy("segment")
)
