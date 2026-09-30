from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.year("delivered_date") >= 2024)
    .groupBy("carrier")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("carrier"))).alias("carrier_values"),
        F.min("cost").alias("min_cost"),
    )
    .orderBy(F.col("carrier_values").desc())
)
