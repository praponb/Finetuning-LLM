from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.month("order_date").isin([1, 3, 11]))
    .groupBy(F.year("order_date").alias("order_year"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("channel"))).alias("channel_values"),
    )
)
