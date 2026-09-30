from pyspark.sql import functions as F

base = (
    df_subscriptions
    .filter((F.col("end_date") > "2023-09-01") | F.col("customer_id").isin([12, 18, 27]))
)

agg = (
    base
    .groupBy("status")
    .agg(F.sort_array(F.collect_set("status")).alias("status_list"))
)

result = agg.orderBy("status")
