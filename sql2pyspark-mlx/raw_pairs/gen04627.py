from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.orderBy("sale_date", "id").rowsBetween(-6, -1)
w2 = Window.orderBy(F.col("revenue").asc_nulls_last())
w3 = Window.orderBy(F.col("units").desc_nulls_first())
w4 = Window.partitionBy(F.date_trunc("week", "sale_date"), "store_id").orderBy(F.col("units").desc(), "id")
w5 = Window.partitionBy("store_id", "product_id").orderBy("sale_date", "id").rowsBetween(-6, -1)

scored = (
    df_store_sales
    .select(
        "id",
        "product_id",
        "units",
        "revenue",
        F.avg("revenue").over(w_recent).alias("revenue_avg_6_prior"),
        F.dense_rank().over(w2).alias("revenue_dense_rank"),
        F.cume_dist().over(w3).alias("units_cume_dist"),
        F.ntile(3).over(w4).alias("units_bucket"),
        F.avg("units").over(w5).alias("units_avg_6_prior"),
    )
)

result = (
    scored
    .groupBy(
        F.when(F.col("units_cume_dist") >= 0.76, "HIGH").otherwise("LOW").alias("units_cume_dist_tier"),
    )
    .agg(
        F.round(F.avg("revenue_avg_6_prior"), 2).alias("avg_revenue_avg_6_prior"),
        F.percentile_approx("units_cume_dist", 0.95).alias("units_cume_dist_p95"),
        F.percentile_approx("units_cume_dist", 0.95).alias("units_cume_dist_p95_2"),
        F.sum("units_cume_dist").alias("total_units_cume_dist"),
        F.max("units_cume_dist").alias("max_units_cume_dist"),
        F.round(F.avg("revenue"), 2).alias("avg_revenue"),
    )
    .filter(F.col("max_units_cume_dist") > 0)
    .orderBy("max_units_cume_dist")
)
