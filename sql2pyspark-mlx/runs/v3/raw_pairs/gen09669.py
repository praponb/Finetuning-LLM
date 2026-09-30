from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("status")
    .agg(
        F.countDistinct("cost").alias("unique_statuss"),
        F.sort_array(F.collect_set("carrier")).alias("carrier_list"),
    )
)
