from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy(F.col("revenue").desc_nulls_last())
w2 = Window.partitionBy("store_id", "product_id")
w3 = Window.partitionBy("product_id").orderBy("sale_date", "id")

scored = (
    df_store_sales
    .filter(F.col("revenue") >= 2800)
    .select(
        "id",
        "store_id",
        F.rank().over(w).alias("revenue_rank"),
        F.round((F.col("units") / F.sum("units").over(w2)), 4).alias("units_share"),
        F.row_number().over(w3).alias("seq_no"),
    )
)

result = (
    scored
    .groupBy(
        F.when(F.col("units_share") >= 0.79, "HIGH").otherwise("LOW").alias("units_share_tier"),
        "seq_no",
    )
    .agg(
        F.percentile_approx("units_share", 0.9).alias("units_share_p90"),
        F.round(F.avg("seq_no"), 2).alias("avg_seq_no"),
    )
    .filter(F.col("units_share_p90") > 0)
    .orderBy("units_share_p90")
)
