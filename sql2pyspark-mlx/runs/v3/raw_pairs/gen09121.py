from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("revenue").desc(), "id")

result = (
    df_store_sales
    .filter(F.col("revenue") >= 3800)
    .select(
        "id",
        "store_id",
        "revenue",
        "units",
        F.row_number().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") <= 3)
    .orderBy(F.col("revenue").desc(), F.col("units").desc())
)
