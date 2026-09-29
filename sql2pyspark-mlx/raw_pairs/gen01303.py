from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("city_length", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy(F.month("opened_date")).orderBy("opened_date", "id")
w3 = Window.orderBy("opened_date", "id").rowsBetween(-3, Window.currentRow)
w_hist = Window.partitionBy(F.date_trunc("YEAR", "opened_date"), "city").orderBy(F.col("opened_date").desc(), "id")
w_part = Window.partitionBy("region").orderBy("city_length", "id")
w6 = Window.orderBy(F.col("prev_city_length").asc_nulls_last())
w7 = Window.orderBy(F.col("row_seq").desc(), "id")
w8 = Window.orderBy("row_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.orderBy("city_length_avg_3")

base = (
    df_stores
    .filter(F.col("region").isNotNull() & (F.length("store_name") > 6))
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        F.length("city").alias("city_length"),
        F.coalesce("region", F.lit("NONE")).alias("region_filled"),
    )
)

metrics = (
    base
    .select(
        "id",
        "city_length",
        "region_filled",
        F.first("city_length", ignorenulls=True).over(w).alias("first_known_city_length"),
        F.lead("city_length", 1).over(w2).alias("next_city_length"),
        F.avg("city_length").over(w3).alias("city_length_avg_3"),
        F.row_number().over(w_hist).alias("row_seq"),
        F.lag("city_length", 2).over(w_part).alias("prev_city_length"),
    )
)

kept = (
    metrics
    .alias("s")
    .join(df_orders.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.channel").isin(["web", "mobile"])), "left_anti")
)

base_stores = (
    kept
    .filter(F.col("first_known_city_length") >= 33)
    .select(
        "*",
        (F.col("first_known_city_length") + F.col("city_length_avg_3")).alias("first_known_city_length_plus_city_length_avg_3"),
    )
)

metrics_stores = (
    base_stores
    .select(
        "*",
        F.percent_rank().over(w6).alias("prev_city_length_percent_rank"),
        F.row_number().over(w7).alias("event_seq"),
        F.sum("city_length").over(w8).alias("cumulative_city_length"),
        F.dense_rank().over(w9).alias("city_length_avg_3_dense_rank"),
    )
)

result = (
    metrics_stores
    .select(
        "id",
        "region_filled",
        "first_known_city_length",
        "next_city_length",
        "city_length_avg_3",
        "row_seq",
        "prev_city_length",
        "first_known_city_length_plus_city_length_avg_3",
        "prev_city_length_percent_rank",
    )
    .orderBy(F.col("row_seq").desc())
)
