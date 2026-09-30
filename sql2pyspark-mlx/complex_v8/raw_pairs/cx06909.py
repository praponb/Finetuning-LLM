from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id_by_start_date_runn = Window.partitionBy("holder_id").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_status = Window.partitionBy("status")
w_holder_id_by_start_date = Window.partitionBy("holder_id").orderBy("start_date", "id")
w_status_holder_id_by_premium = Window.partitionBy("status", "holder_id").orderBy(F.col("premium").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_holder_id_by_start_date_full = Window.partitionBy("holder_id").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_status_holder_id_by_premium_2 = Window.partitionBy("status", "holder_id").orderBy(F.col("premium").desc_nulls_first(), "id")

base = (
    df_policies
    .filter((F.col("status") == "LAPSED") & (F.col("start_date").isNotNull()))
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "status",
    )
)

windowed = (
    base
    .select(
        "*",
        F.sum("premium").over(w_holder_id_by_start_date_runn).alias("running_premium"),
        F.sum("premium").over(w_status).alias("status_sum_premium"),
        F.ntile(5).over(w_holder_id_by_start_date).alias("start_date_quintile"),
        F.last("premium", ignorenulls=True).over(w_status_holder_id_by_premium).alias("last_known_premium"),
        F.first("start_date").over(w_holder_id_by_start_date_full).alias("first_start_date"),
        F.datediff(F.col("start_date"), F.lag("start_date").over(w_status_holder_id_by_premium_2)).alias("days_since_prev_start_date"),
        F.row_number().over(w_status_holder_id_by_premium_2).alias("rn"),
        F.rank().over(w_status_holder_id_by_premium_2).alias("premium_rank"),
        F.round(F.avg("premium").over(w_status), 2).alias("status_avg_premium"),
    )
)

annotated = (
    windowed
    .select(
        "id",
        "premium",
        "start_date",
        "status",
        "running_premium",
        "last_known_premium",
        "first_start_date",
        F.datediff(F.col("start_date"), F.col("first_start_date")).alias("days_first_start_date_to_start_date"),
    )
)

result = (
    annotated
    .select(
        "id",
        "premium",
        "start_date",
        "status",
        "running_premium",
        "last_known_premium",
        "first_start_date",
        "days_first_start_date_to_start_date",
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
        F.concat(F.col("status"), F.lit("#"), F.col("id").cast("string")).alias("status_ref"),
    )
    .orderBy(F.col("premium").desc_nulls_first())
)
