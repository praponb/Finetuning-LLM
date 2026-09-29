from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_region_by_end_date_fu = Window.partitionBy("holder_region").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_status_by_holder_joined_on_f = Window.partitionBy("status").orderBy("holder_joined_on", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_holder_joined_on_month_by_pr = Window.partitionBy(F.date_trunc("month", "holder_joined_on")).orderBy("premium", "id").rowsBetween(-3, Window.currentRow)
w_holder_region_by_end_date_ru = Window.partitionBy("holder_region").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_status_product = Window.partitionBy("status", "product")
w_status_by_holder_joined_on_m = Window.partitionBy("status").orderBy("holder_joined_on", "id").rowsBetween(-3, Window.currentRow)

base = (
    df_policies
    .filter(F.col("premium") > 2289.0)
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
    )
)

with_details_holder = (
    base.alias("s")
    .join(df_policyholders.alias("p"), (F.col("p.id") == F.col("s.holder_id")) & (F.col("p.joined_on") < F.lit("2024-12-21").cast("date")), "inner")
    .filter(F.col("p.region").isin("Central", "North"))
    .select(
        "s.*",
        "p.holder_name",
        F.col("p.joined_on").alias("holder_joined_on"),
        F.col("p.region").alias("holder_region"),
    )
)

windowed = (
    with_details_holder
    .select(
        "*",
        F.last("premium").over(w_holder_region_by_end_date_fu).alias("last_premium"),
        F.nth_value("holder_joined_on", 2).over(w_holder_region_by_end_date_fu).alias("second_holder_joined_on"),
        F.last("holder_joined_on").over(w_status_by_holder_joined_on_f).alias("last_holder_joined_on"),
        F.round(F.avg("premium").over(w_holder_joined_on_month_by_pr), 2).alias("premium_avg_4"),
        F.min("premium").over(w_holder_region_by_end_date_ru).alias("min_premium_so_far"),
        F.count(F.lit(1)).over(w_status_product).alias("rows_in_status_product"),
        F.max("premium").over(w_status_by_holder_joined_on_m).alias("premium_max_4"),
        F.size(F.collect_set("holder_id").over(w_status_product)).alias("distinct_holder_id_in_status_product"),
        F.last("holder_joined_on").over(w_holder_region_by_end_date_fu).alias("last_holder_joined_on_3"),
    )
)

result = (
    windowed
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        "status",
        "holder_name",
        "holder_region",
        "last_premium",
        "second_holder_joined_on",
        "last_holder_joined_on",
        "min_premium_so_far",
        "rows_in_status_product",
        "distinct_holder_id_in_status_product",
        "last_holder_joined_on_3",
        F.year(F.col("end_date")).alias("end_date_year"),
        F.lower(F.regexp_replace(F.trim(F.col("holder_region")), " ", "_")).alias("holder_region_slug"),
    )
)
