from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "city")
w2 = Window.partitionBy("country")
w3 = Window.orderBy("rating", "id").rowsBetween(-2, Window.currentRow)
w4 = Window.partitionBy("city").orderBy(F.col("rating").asc_nulls_first())
w5 = Window.orderBy("rating_share", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("country").orderBy(F.col("rating_share").desc_nulls_first())
w7 = Window.partitionBy("city").orderBy(F.col("rating_share").desc(), "id")
w8 = Window.partitionBy("country").orderBy("rating_share_dense_rank", "id")
w9 = Window.partitionBy("city", "country")
w10 = Window.partitionBy("city", "country").orderBy(F.col("country_max_rating").desc(), "id")
w11 = Window.partitionBy("country").orderBy(F.col("city_sum_country_max_rating").desc(), "id")

scored = (
    df_suppliers
    .withColumn("rating_share", F.round((F.col("rating") / F.sum("rating").over(w)), 4))
    .withColumn("country_max_rating", F.max("rating").over(w2))
    .withColumn("rating_avg_2", F.avg("rating").over(w3))
    .withColumn("rating_percent_rank", F.percent_rank().over(w4))
)

scored_suppliers = (
    scored
    .withColumn("cumulative_rating_avg_2", F.sum("rating_avg_2").over(w5))
    .withColumn("rating_share_dense_rank", F.dense_rank().over(w6))
)

latest = (
    scored_suppliers
    .withColumn("row_num", F.row_number().over(w7))
    .filter(F.col("row_num") == 1)
)

scored_suppliers_2 = (
    latest
    .filter(F.col("country_max_rating") <= 2)
    .withColumn("next_country_max_rating", F.lead("country_max_rating", 2).over(w8))
    .withColumn("city_sum_country_max_rating", F.sum("country_max_rating").over(w9))
)

eligible = (
    scored_suppliers_2
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.stock_qty") > 162), "left_anti")
)

deduped = (
    eligible
    .withColumn("rank_in_group", F.row_number().over(w10))
    .filter(F.col("rank_in_group") == 1)
)

latest_suppliers = (
    deduped
    .withColumn("rank_in_group_2", F.row_number().over(w11))
    .filter(F.col("rank_in_group_2") == 1)
    .select(
        "id",
        "city",
        "rating_share_dense_rank",
        "row_num",
        "city_sum_country_max_rating",
    )
)

result = (
    latest_suppliers
    .select("id", "city", "city_sum_country_max_rating")
    .orderBy("city_sum_country_max_rating")
)
