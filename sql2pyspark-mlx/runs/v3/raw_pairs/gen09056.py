from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date") > "2025-04-17")
    .groupBy(F.date_trunc("MONTH", "delivered_date").alias("delivered_month"), "status")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("carrier"))).alias("carrier_values"),
        F.sort_array(F.collect_set("carrier")).alias("carrier_list"),
    )
    .orderBy("carrier_values")
)
