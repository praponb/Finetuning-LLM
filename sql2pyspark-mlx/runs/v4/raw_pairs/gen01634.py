from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("order_id", "status").orderBy(F.col("amount").asc_nulls_first())

enriched = (
    df_payments
    .select(
        "*",
        F.sum("amount").over(w).alias("running_amount"),
        F.cume_dist().over(w2).alias("amount_cume_dist"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("amount_cume_dist") >= 0.89, "LARGE").otherwise("SMALL").alias("amount_cume_dist_tier"),
    )
    .agg(
        F.countDistinct("method").alias("distinct_methods"),
        F.countDistinct("order_id").alias("distinct_order_ids"),
        F.percentile_approx("amount_cume_dist", [0.25, 0.5, 0.75]).alias("amount_cume_dist_quartiles"),
        F.sum("amount").alias("total_amount"),
    )
)
