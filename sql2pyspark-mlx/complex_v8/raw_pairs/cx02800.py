from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_city = Window.partitionBy("city")
w_city_by_opened_on = Window.partitionBy("city").orderBy("opened_on", "id")
w_city_by_opened_on_full = Window.partitionBy("city").orderBy("opened_on", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped_warehouses = (
    df_warehouses
    .filter((F.col("city") == "Kuala Lumpur") & (F.lower(F.col("wh_code")).like("%k1%")))
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        "opened_on",
        F.substring(F.trim(F.col("wh_code")), 1, 3).alias("wh_code_prefix"),
    )
)

sequenced = (
    scoped_warehouses
    .select(
        "*",
        F.round(F.col("capacity") / F.nullif(F.sum("capacity").over(w_city), F.lit(0)), 4).alias("capacity_share_of_city"),
        F.ntile(10).over(w_city_by_opened_on).alias("opened_on_decile"),
        F.last("opened_on").over(w_city_by_opened_on_full).alias("last_opened_on"),
        F.round(F.col("capacity") - F.avg("capacity").over(w_city), 2).alias("capacity_vs_city_avg"),
    )
)

enriched = (
    sequenced
    .select(
        "id",
        "wh_code",
        "capacity",
        "opened_on",
        "wh_code_prefix",
        "opened_on_decile",
        "last_opened_on",
        F.coalesce(F.col("city"), F.lit("UNKNOWN")).alias("city_or_unknown"),
        F.date_trunc("month", F.col("last_opened_on")).alias("last_opened_on_month"),
    )
)

result = (
    enriched
    .select("id", "capacity", "opened_on_decile")
    .unpivot(
        ids=["id"],
        values=["capacity", "opened_on_decile"],
        variableColumnName="measure",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
