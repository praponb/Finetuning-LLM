from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_start_date_quarter_holder_id = Window.partitionBy("start_date_quarter", "holder_id").orderBy(F.col("start_date_quarter").desc(), "id")
w_start_date_quarter_end_date = Window.partitionBy("start_date_quarter", F.date_trunc("month", "end_date"))
w_product_holder_id_by_premium = Window.partitionBy("product", "holder_id").orderBy("premium", "id")
w_product = Window.partitionBy("product")
w_start_date_quarter_holder_id_2 = Window.partitionBy("start_date_quarter", "holder_id").orderBy(F.col("start_date_quarter").desc())
w_start_date_quarter_product = Window.partitionBy("start_date_quarter", "product")
w_premium_dense_rank_by_produc = Window.partitionBy("premium_dense_rank").orderBy("product_avg_premium", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_premium_dense_rank = Window.partitionBy("premium_dense_rank")

filtered_policys = (
    df_policies
    .filter((F.col("start_date") >= F.lit("2024-12-03").cast("date")) & (F.col("status").isin("CANCELLED", "LAPSED", "ACTIVE")))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
        F.quarter(F.col("start_date")).alias("start_date_quarter"),
    )
)

windowed = (
    filtered_policys
    .select(
        "id",
        "product",
        "end_date",
        F.ntile(10).over(w_start_date_quarter_holder_id).alias("start_date_quarter_decile"),
        F.max("start_date_quarter").over(w_start_date_quarter_end_date).alias("start_date_quarter_end_date_month_max_start_date"),
        (F.col("start_date_quarter") - F.lag("start_date_quarter").over(w_product_holder_id_by_premium)).alias("start_date_quarter_change"),
        F.max("start_date_quarter").over(w_product).alias("product_max_start_date_quarter"),
        F.rank().over(w_start_date_quarter_holder_id).alias("start_date_quarter_rank"),
        F.round(F.avg("premium").over(w_product), 2).alias("product_avg_premium"),
        F.dense_rank().over(w_product_holder_id_by_premium).alias("premium_dense_rank"),
        F.dense_rank().over(w_start_date_quarter_holder_id_2).alias("start_date_quarter_dense_rank"),
        F.min("premium").over(w_start_date_quarter_product).alias("start_date_quarter_product_min_premium"),
    )
)

with_windows = (
    windowed
    .select(
        "*",
        F.last("product_avg_premium", ignorenulls=True).over(w_premium_dense_rank_by_produc).alias("last_known_product_avg_premium"),
        F.max("start_date_quarter_decile").over(w_premium_dense_rank).alias("premium_dense_rank_max_start_date_quarter_decile"),
        F.round(F.col("start_date_quarter_rank") / F.nullif(F.sum("start_date_quarter_rank").over(w_premium_dense_rank), F.lit(0)), 4).alias("start_date_quarter_rank_share_of_premium_dense_r"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "product",
        "end_date",
        "start_date_quarter_end_date_month_max_start_date",
        "start_date_quarter_change",
        "product_max_start_date_quarter",
        "product_avg_premium",
        "premium_dense_rank",
        "start_date_quarter_product_min_premium",
        "premium_dense_rank_max_start_date_quarter_decile",
        "start_date_quarter_rank_share_of_premium_dense_r",
    )
)
