from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("month", "opened_date"), "region").orderBy("opened_date", "id")
w2 = Window.partitionBy("region").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_windows = (
    df_stores
    .select(
        "id",
        "store_name",
        "region",
        "opened_date",
        F.lead("sqft", 1).over(w).alias("next_sqft"),
        F.sum("sqft").over(w2).alias("running_sqft"),
    )
)

result = (
    with_windows
    .select("id", "store_name", "region", "running_sqft")
    .orderBy(F.col("region").asc_nulls_last())
)
