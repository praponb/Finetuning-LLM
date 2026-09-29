from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.orderBy(F.col("units").desc_nulls_last())
w_part_2 = Window.partitionBy("store_id")
w3 = Window.partitionBy("store_id").orderBy("sale_date", "id")
w4 = Window.partitionBy("product_id", "store_id").orderBy("sale_date", "id")
w5 = Window.partitionBy("store_id").orderBy("units_rank", "id")
w_grp = Window.partitionBy("product_id").orderBy(F.col("event_seq").desc(), "id")
w7 = Window.partitionBy("store_id").orderBy("revenue", "id")

scored = (
    df_store_sales
    .withColumn("units_rank", F.rank().over(w_part))
    .withColumn("store_id_sum_units", F.sum("units").over(w_part_2))
    .withColumn("event_seq", F.row_number().over(w3))
)

deduped = (
    scored
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
)

enriched = (
    deduped
    .withColumn("prev_units", F.lag("units", 2).over(w5))
    .withColumn("event_seq_bucket", F.ntile(4).over(w_grp))
    .withColumn("seq_no", F.row_number().over(w7))
)

result = (
    enriched
    .select(
        "id",
        "store_id",
        "sale_date",
        "units",
        "revenue",
        "units_rank",
        "store_id_sum_units",
        "rank_in_group",
        "prev_units",
    )
)
