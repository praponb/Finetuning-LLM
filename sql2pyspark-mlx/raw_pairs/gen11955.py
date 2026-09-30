from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("YEAR", "effective_from"), "plan").orderBy(F.col("element").desc_nulls_last())
w2 = Window.orderBy(F.col("element").asc_nulls_last())
w_ordered = Window.partitionBy("plan")
w_recent = Window.partitionBy("currency", "plan")

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("discounts").alias("element"),
    )
)

result = (
    lines
    .filter(F.col("effective_from").between("2023-04-15", "2024-10-01"))
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        "element",
        F.cume_dist().over(w).alias("element_cume_dist"),
        F.dense_rank().over(w2).alias("element_dense_rank"),
        F.count("element").over(w_ordered).alias("plan_count_element"),
        F.min("element").over(w_recent).alias("currency_min_element"),
        F.sum("element").over(w_ordered).alias("plan_sum_element"),
    )
)
