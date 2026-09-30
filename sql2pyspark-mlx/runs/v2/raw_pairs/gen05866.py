from pyspark.sql import functions as F

src = (
    df_shipments
    .filter(F.month("delivered_date").isin([4, 8, 12]) & (F.col("cost") <= 37))
)

status_summary = (
    src
    .groupBy("status")
    .agg(
        F.sort_array(F.collect_set("carrier")).alias("carrier_list"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
)

result = status_summary.orderBy("status")
