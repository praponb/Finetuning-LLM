from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(F.quarter("delivered_date").alias("delivered_quarter"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("carrier"))).alias("carrier_values"),
        F.count("cost").alias("cost_count"),
    )
)
