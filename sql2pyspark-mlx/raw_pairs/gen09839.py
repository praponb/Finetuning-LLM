from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("revenue", "id")
w2 = Window.partitionBy("store_id")

enriched = (
    df_store_sales
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        F.lag("units").over(w).alias("prev_units"),
        F.lag("revenue").over(w).alias("prev_revenue"),
        F.lead("revenue").over(w).alias("next_revenue"),
        F.row_number().over(w).alias("event_seq"),
        F.avg("revenue").over(w2).alias("store_id_avg_revenue"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("prev_revenue") >= 4000, "HIGH").otherwise("LOW").alias("prev_revenue_tier"),
    )
    .agg(
        F.percentile_approx("event_seq", 0.5).alias("event_seq_p50"),
        F.round(F.avg("event_seq"), 2).alias("avg_event_seq"),
        F.min("prev_revenue").alias("min_prev_revenue"),
    )
    .filter(F.col("event_seq_p50") < 1000000)
    .orderBy("event_seq_p50")
)
