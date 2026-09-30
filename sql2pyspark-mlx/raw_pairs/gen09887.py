from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city").isin(["Chiang Mai", "Bangkok"]))
    .select("city", F.date_format("opened_date", "yyyy-MM").alias("opened_date_label"))
)
