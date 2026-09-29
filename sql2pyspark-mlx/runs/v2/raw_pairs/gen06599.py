from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.month("signup_date").isin([3, 6, 11]) & (F.col("country") != "DE")) | F.col("age").between(37, 45))
    .agg(F.sort_array(F.collect_set("segment")).alias("segment_list"))
)
