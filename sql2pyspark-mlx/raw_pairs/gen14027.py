from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("city").orderBy(F.col("sqft").desc())
w_recent = Window.partitionBy("region").orderBy("sqft", "id")

with_windows = (
    df_stores
    .select(
        "*",
        F.cume_dist().over(w_grp).alias("sqft_cume_dist"),
        F.ntile(4).over(w_recent).alias("sqft_bucket"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("sqft") >= 17643, "LARGE").otherwise("SMALL").alias("sqft_tier"),
    )
    .agg(
        F.percentile_approx("sqft_bucket", 0.95).alias("sqft_bucket_p95"),
        F.sum("sqft_cume_dist").alias("total_sqft_cume_dist"),
    )
)
