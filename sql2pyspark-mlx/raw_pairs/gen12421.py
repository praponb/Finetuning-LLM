from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("cost").between(31, 37))
    .groupBy("order_id")
    .agg(F.sort_array(F.collect_set("carrier")).alias("carrier_list"))
    .orderBy(F.col("order_id").desc())
)
