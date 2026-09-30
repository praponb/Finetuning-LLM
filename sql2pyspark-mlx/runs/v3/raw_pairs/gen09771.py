from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("segment").isin(["SMB", "Consumer", "Corporate"]) | F.col("name").like("Jac%"))
    .withColumn("segment_country_key", F.concat_ws("-", "segment", "country"))
)
