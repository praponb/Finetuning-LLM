from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("city").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("region").orderBy("sqft", "id")
w3 = Window.partitionBy("region")

result = (
    df_stores
    .select(
        "*",
        F.first("region", ignorenulls=True).over(w_part).alias("first_known_region"),
        F.row_number().over(w_hist).alias("row_seq"),
        F.round((F.col("sqft") / F.sum("sqft").over(w3)), 4).alias("sqft_share"),
        F.ntile(4).over(w_hist).alias("sqft_bucket"),
    )
)
