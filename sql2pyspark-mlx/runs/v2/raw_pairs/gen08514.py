from pyspark.sql import functions as F

result = (
    df_stores
    .filter(((F.year("opened_date") >= 2024) & (F.upper(F.trim("store_name")) == "SIAM FLAGSHIP")) | F.col("city").isin(["Berlin", "Bangkok", "Singapore"]))
    .agg(F.min("opened_date").alias("first_opened_date"))
    .orderBy("first_opened_date")
)
