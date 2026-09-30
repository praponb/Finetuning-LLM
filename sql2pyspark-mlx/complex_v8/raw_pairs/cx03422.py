from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product_holder_joined_on_mon = Window.partitionBy("product", F.date_trunc("month", "holder_joined_on")).orderBy("premium", "id")
w_product_by_start_date = Window.partitionBy("product").orderBy("start_date", "id")
w_start_date_year = Window.partitionBy(F.year("start_date"))
w_holder_id_holder_joined_on_m = Window.partitionBy("holder_id", F.month("holder_joined_on")).orderBy("holder_joined_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_holder_id_holder_joined_on_m_2 = Window.partitionBy("holder_id", F.month("holder_joined_on")).orderBy("holder_joined_on", "id")
w_product_holder_joined_on_mon_2 = Window.partitionBy("product", F.date_trunc("month", "holder_joined_on")).orderBy("premium")

valid_policys = (
    df_policies
    .filter(F.col("end_date") < F.lit("2026-10-23").cast("date"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        F.lower(F.trim(F.col("status"))).alias("status_clean"),
    )
)

with_details_holder = (
    valid_policys.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select("s.*", F.col("p.joined_on").alias("holder_joined_on"))
)

sequenced = (
    with_details_holder
    .select(
        "id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status_clean",
        "holder_joined_on",
        F.lead("end_date").over(w_product_holder_joined_on_mon).alias("next_end_date"),
        F.ntile(10).over(w_product_by_start_date).alias("start_date_decile"),
        F.sum("premium").over(w_start_date_year).alias("start_date_year_sum_premium"),
        F.round(F.percent_rank().over(w_product_holder_joined_on_mon), 3).alias("premium_percent_rank"),
        F.last("start_date", ignorenulls=True).over(w_holder_id_holder_joined_on_m).alias("last_known_start_date"),
        F.last("holder_joined_on", ignorenulls=True).over(w_holder_id_holder_joined_on_m).alias("last_known_holder_joined_on"),
        F.lag("start_date").over(w_holder_id_holder_joined_on_m_2).alias("prev_start_date"),
        F.rank().over(w_product_holder_joined_on_mon_2).alias("premium_rank"),
        F.round(F.percent_rank().over(w_holder_id_holder_joined_on_m_2), 3).alias("holder_joined_on_percent_rank"),
        F.size(F.collect_set("holder_id").over(w_start_date_year)).alias("distinct_holder_id_in_start_date_year"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_start_date_year), F.lit(0)), 4).alias("premium_share_of_start_date_year"),
    )
)

first_rows = (
    sequenced
    .filter((F.col("premium_rank") == 1) & (F.col("holder_joined_on_percent_rank") >= 0.1))
    .drop("premium_rank")
)

result = (
    first_rows
    .select(
        "id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status_clean",
        "holder_joined_on",
        "start_date_decile",
        "start_date_year_sum_premium",
        "prev_start_date",
        "distinct_holder_id_in_start_date_year",
        "premium_share_of_start_date_year",
        (F.floor(F.col("premium") / 1000) * 1000).alias("premium_bucket"),
        F.coalesce(F.col("product"), F.lit("UNKNOWN")).alias("product_or_unknown"),
    )
    .orderBy(F.col("end_date").desc(), F.col("product").desc_nulls_first())
)
