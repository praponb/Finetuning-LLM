from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("carrier") == "UPS") & (F.col("carrier") == "UPS"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("carrier") == "UPS", 1).otherwise(0))) / F.count("*")), 1).alias("pct_ups"),
        F.sort_array(F.collect_set("carrier")).alias("carrier_list"),
    )
    .orderBy("pct_ups")
)
