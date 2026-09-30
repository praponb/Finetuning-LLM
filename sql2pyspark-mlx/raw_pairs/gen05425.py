from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("last_restock", "id")
w2 = Window.partitionBy("store_id").orderBy("last_restock", "id").rowsBetween(-6, -1)

scored = (
    df_inventory
    .filter(F.col("last_restock").between("2023-09-11", "2025-05-01"))
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("quantity_avg_6_prior", F.avg("quantity").over(w2))
)

result = (
    scored
    .groupBy(
        F.when(F.col("quantity_avg_6_prior") >= 263, "PREMIUM").otherwise("STANDARD").alias("quantity_avg_6_prior_tier"),
        "store_id",
    )
    .agg(
        F.percentile_approx("seq_no", 0.9).alias("seq_no_p90"),
        F.count("*").alias("cnt"),
        F.countDistinct("product_id").alias("distinct_product_ids"),
        F.percentile_approx("seq_no", 0.5).alias("seq_no_p50"),
        F.stddev_samp("quantity").alias("quantity_stddev"),
    )
    .filter(F.col("cnt") > 1)
)
