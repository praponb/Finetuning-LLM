from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("opened_date", "id")
w2 = Window.partitionBy("store_id").orderBy(F.col("opened_date").asc_nulls_last(), "id")
w3 = Window.partitionBy("store_id").orderBy(F.col("units").asc_nulls_last())
w4 = Window.orderBy(F.col("rank_in_group").desc(), "id")
w5 = Window.partitionBy(F.year("opened_date")).orderBy("rank_in_group", "id")
w6 = Window.partitionBy("city", "store_id").orderBy("units", "id").rowsBetween(-2, -1)
w7 = Window.partitionBy("store_id").orderBy(F.col("units").desc_nulls_first())
w8 = Window.partitionBy("store_id").orderBy("units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.orderBy("row_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_store_sales
    .filter((F.col("sale_date") > F.to_date(F.lit("2023-07-18"))) & (F.col("units") > 26))
    .select(
        "id",
        "store_id",
        "sale_date",
        "units",
        "revenue",
        F.when(F.col("revenue") >= 3500, "High").when(F.col("revenue") >= 900, "Medium").otherwise("Low").alias("revenue_level"),
        F.date_add("sale_date", 83).alias("sale_date_plus_83d"),
    )
)

enriched = (
    filtered
    .alias("a")
    .join(df_stores.alias("b"), F.col("a.store_id") == F.col("b.id"), "left")
    .select("a.id", "a.store_id", "a.units", F.col("b.opened_date"), F.col("b.city"))
)

deduped = (
    enriched
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

deduped_store_sales = (
    deduped
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
)

enriched_store_sales = (
    deduped_store_sales
    .select(
        "id",
        "store_id",
        "units",
        F.rank().over(w3).alias("units_rank"),
        F.row_number().over(w4).alias("row_seq"),
        F.lead("units", 1, 0).over(w5).alias("next_units"),
    )
)

enriched_store_sales_2 = (
    enriched_store_sales
    .alias("a")
    .join(df_stores.alias("s"), F.col("a.store_id") == F.col("s.id"), "left")
    .filter(F.col("s.city") == "Berlin")
    .select(
        "a.id",
        "a.store_id",
        "a.units",
        "a.row_seq",
        "a.next_units",
        F.col("s.city"),
        F.col("s.store_name"),
    )
)

scored = (
    enriched_store_sales_2
    .select(
        "*",
        F.avg("row_seq").over(w6).alias("row_seq_avg_2_prior"),
        F.dense_rank().over(w7).alias("units_dense_rank"),
        F.last("row_seq", ignorenulls=True).over(w8).alias("last_known_row_seq"),
        F.sum("next_units").over(w9).alias("running_next_units"),
    )
)

result = (
    scored
    .groupBy("store_id")
    .pivot("city", ["Chiang Mai", "Singapore"])
    .agg(F.max("row_seq").alias("total"))
    .select(
        "store_id",
        F.col("Chiang Mai").alias("chiang_mai"),
        F.col("Singapore").alias("singapore"),
    )
)
