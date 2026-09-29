from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("sqft", "id").rowsBetween(-5, -1)
w2 = Window.partitionBy(F.year("opened_date"), "city").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("city").orderBy("opened_date", "id")

result = (
    df_stores
    .select(
        "id",
        "store_name",
        "city",
        "sqft",
        F.avg("sqft").over(w).alias("sqft_avg_5_prior"),
        F.last("region", ignorenulls=True).over(w2).alias("last_known_region"),
        F.sum("sqft").over(w3).alias("cumulative_sqft"),
        F.row_number().over(w4).alias("row_seq"),
    )
)
