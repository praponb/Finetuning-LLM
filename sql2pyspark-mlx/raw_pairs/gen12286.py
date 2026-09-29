from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("quantity", "id")
w_recent = Window.partitionBy("store_id")

metrics = (
    df_inventory
    .withColumn("next_quantity", F.lead("quantity", 2, 0).over(w))
    .withColumn("store_id_stddev_samp_quantity", F.stddev_samp("quantity").over(w_recent))
)

result = (
    metrics
    .groupBy(
        F.when(F.col("quantity") >= 180, "LARGE").otherwise("SMALL").alias("quantity_tier"),
    )
    .agg(
        F.percentile_approx("next_quantity", 0.95).alias("next_quantity_p95"),
        F.bit_and("quantity").alias("quantity_bit_and"),
        F.percentile_approx("store_id_stddev_samp_quantity", [0.25, 0.5, 0.75]).alias("store_id_stddev_samp_quantity_quartiles"),
        F.countDistinct("store_id").alias("distinct_store_ids"),
        F.max("store_id_stddev_samp_quantity").alias("max_store_id_stddev_samp_quantity"),
    )
    .filter(F.col("next_quantity_p95") > 0)
    .orderBy("next_quantity_p95")
)
