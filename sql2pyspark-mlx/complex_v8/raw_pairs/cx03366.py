from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product_by_days_since_end_da = Window.partitionBy("product").orderBy(F.col("days_since_end_date").desc())
w_holder_id_status_by_end_date = Window.partitionBy("holder_id", "status").orderBy(F.col("end_date").desc(), "id")

cleaned_policys = (
    df_policies
    .filter((F.col("status") == "ACTIVE") & (F.col("product") == "HOME"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.substring(F.trim(F.col("product")), 1, 2).alias("product_prefix"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("end_date")).alias("days_since_end_date"),
    )
)

sequenced = (
    cleaned_policys
    .select(
        "*",
        F.rank().over(w_product_by_days_since_end_da).alias("days_since_end_date_rank"),
        F.lag("days_since_end_date").over(w_holder_id_status_by_end_date).alias("prev_days_since_end_date"),
        F.lag("start_date").over(w_holder_id_status_by_end_date).alias("prev_start_date"),
    )
)

totals = (
    sequenced
    .filter(F.col("product_prefix").isNotNull())
    .groupBy(F.date_trunc("month", "end_date").alias("end_date_month"))
    .agg(
        F.count("*").alias("n_rows"),
        F.max("start_date").alias("last_start_date"),
        F.round(F.sum("prev_days_since_end_date") / F.nullif(F.sum("days_since_end_date"), F.lit(0)), 3).alias("prev_days_since_end_date_to_days_since_end_date_"),
    )
    .filter(F.col("n_rows") >= 3)
)

result = (
    totals
    .select(
        "end_date_month",
        "last_start_date",
        "prev_days_since_end_date_to_days_since_end_date_",
    )
    .orderBy("end_date_month", F.col("last_start_date").asc())
    .limit(5)
)
