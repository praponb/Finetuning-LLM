from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("store_id").orderBy("units", "id")
w4 = Window.partitionBy("city").orderBy("sqft", "id")

valid = (
    df_store_sales
    .filter(F.col("revenue").between(1800, 3300))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        F.round((F.col("units") * 0.9), 2).alias("units_scaled"),
        F.round("units", 1).alias("units_rounded"),
    )
)

with_dims = (
    valid
    .alias("e")
    .join(df_stores.alias("p"), F.col("e.store_id") == F.col("p.id"), "left")
    .filter(F.month(F.col("p.opened_date")).isin([1, 10, 11]))
    .select("e.*", F.col("p.city"), F.col("p.sqft"), F.col("p.store_name"))
)

scored = (
    with_dims
    .select(
        "id",
        "store_id",
        "units",
        "revenue",
        "units_scaled",
        "city",
        "sqft",
        F.sum("sqft").over(w).alias("running_sqft"),
        F.first("units_scaled", ignorenulls=True).over(w2).alias("first_known_units_scaled"),
    )
)

scored_store_sales = (
    scored
    .select(
        "id",
        "revenue",
        "city",
        "sqft",
        "first_known_units_scaled",
        F.lag("running_sqft").over(w_grp).alias("prev_running_sqft"),
        F.lag("first_known_units_scaled").over(w_grp).alias("prev_first_known_units_scaled"),
        F.row_number().over(w_grp).alias("row_seq"),
        F.lag("revenue", 2).over(w4).alias("prev_revenue"),
    )
)

result = (
    scored_store_sales
    .select(
        "id",
        "city",
        "first_known_units_scaled",
        "prev_running_sqft",
        "row_seq",
        "prev_revenue",
        F.when(F.col("city").rlike("^(London|Tokyo)$"), "GROUP_B").otherwise("PRIORITY").alias("city_zone"),
    )
)
