from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("revenue").desc(), "id")
w_ordered = Window.orderBy(F.col("revenue").asc_nulls_first())
w3 = Window.partitionBy("store_id")
w4 = Window.partitionBy("product_id").orderBy(F.col("store_id_avg_revenue").desc_nulls_last(), "id")

valid = (
    df_store_sales
    .filter(F.col("product_id").isin([2, 5, 7, 9]))
    .select("*", F.round((F.col("revenue") * 1.15), 2).alias("revenue_adjusted"))
)

deduped = (
    valid
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

scored = (
    deduped
    .select(
        "id",
        "product_id",
        "sale_date",
        "revenue",
        "revenue_adjusted",
        "rn",
        F.dense_rank().over(w_ordered).alias("revenue_dense_rank"),
        F.avg("revenue").over(w3).alias("store_id_avg_revenue"),
    )
)

deduped_store_sales = (
    scored
    .withColumn("rn_2", F.row_number().over(w4))
    .filter(F.col("rn_2") == 1)
    .drop("rn_2")
)

result = (
    deduped_store_sales
    .select("id", "product_id", "sale_date", "revenue", "revenue_adjusted", "rn")
)
