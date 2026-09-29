from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "order_id").orderBy("quantity", "id")
w2 = Window.partitionBy("order_id", "product_id").orderBy(F.col("unit_price").asc_nulls_last())
w_ordered = Window.partitionBy("order_id").orderBy(F.col("quantity").desc(), "id")

enriched = (
    df_order_items
    .select(
        "id",
        "order_id",
        F.row_number().over(w).alias("seq_no"),
        F.dense_rank().over(w2).alias("unit_price_dense_rank"),
        F.row_number().over(w_ordered).alias("event_seq"),
    )
)

result = (
    enriched
    .withColumn("seq_no_tier", F.when(F.col("seq_no") >= 9, "HIGH").otherwise("LOW"))
    .cube("seq_no_tier")
    .agg(
        F.count("*").alias("num_rows"),
        F.count("*").alias("num_rows_2"),
        F.count("*").alias("event_count"),
        F.percentile_approx("seq_no", [0.25, 0.5, 0.75]).alias("seq_no_quartiles"),
        F.count("*").alias("num_rows_3"),
        F.grouping_id().alias("grouping_level"),
    )
    .filter(F.col("event_count") > 1)
)
