from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region").isin(["North", "Central", "East"]))
    .groupBy(F.date_trunc("MONTH", "opened_date").alias("opened_month"))
    .agg(F.sort_array(F.collect_set("region")).alias("region_list"))
)
