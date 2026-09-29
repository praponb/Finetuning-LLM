from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("sqft", "id")
w2 = Window.orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_stores
    .filter(F.col("region").isin(["East", "South", "Central"]))
    .select(
        "id",
        "store_name",
        "region",
        "opened_date",
        "sqft",
        F.concat_ws("-", "store_name", "region").alias("store_name_region_key"),
        F.coalesce("region", F.lit("DIRECT")).alias("resolved_region"),
    )
)

result = (
    filtered
    .select(
        "id",
        "store_name",
        "opened_date",
        "store_name_region_key",
        "resolved_region",
        F.lead("sqft", 1, 0).over(w).alias("next_sqft"),
        F.first("sqft", ignorenulls=True).over(w2).alias("first_known_sqft"),
        F.sum("sqft").over(w2).alias("cumulative_sqft"),
    )
)
