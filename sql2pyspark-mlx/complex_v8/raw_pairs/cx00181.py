from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("product").orderBy(F.col("premium").desc(), F.col("id").desc())

scoped_policys = (
    df_policies
    .filter((F.col("holder_id") <= 18) & (F.col("product").isin("TRAVEL", "AUTO", "HOME")))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        F.round(F.col("premium") * 0.25, 2).alias("premium_adj"),
        (F.floor(F.col("premium") / 1000) * 1000).alias("premium_bucket"),
    )
)

unique_rows = (
    scoped_policys
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    unique_rows
    .cube("holder_id", "product")
    .agg(
        F.count(F.when(F.col("product").isin("HOME", "AUTO", "LIFE"), 1)).alias("n_product_in_home_auto_life"),
    )
    .orderBy(F.col("holder_id").asc_nulls_last(), F.col("product").asc_nulls_last())
)
