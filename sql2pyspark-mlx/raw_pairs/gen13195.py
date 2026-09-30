from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("region").orderBy(F.col("sqft").desc(), "id")
w_hist = Window.partitionBy("region").orderBy(F.col("sqft").desc())
w3 = Window.partitionBy("region").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_stores
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        F.row_number().over(w_part).alias("seq_no"),
        F.cume_dist().over(w_hist).alias("sqft_cume_dist"),
        F.last("region", ignorenulls=True).over(w3).alias("last_known_region"),
        F.last("sqft", ignorenulls=True).over(w4).alias("last_known_sqft"),
    )
)
