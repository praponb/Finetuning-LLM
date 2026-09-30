from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("sale_date", "id")

result = (
    df_store_sales
    .filter(F.col("units").isNull())
    .select(
        "id",
        "store_id",
        "sale_date",
        "revenue",
        F.lag("revenue", 1).over(w).alias("prev_revenue"),
    )
    .orderBy(F.col("store_id").desc())
)
