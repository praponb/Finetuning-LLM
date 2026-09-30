from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("duration_sec", "id")

result = (
    df_web_events
    .filter(F.col("page").isin(["/search", "/checkout", "/product"]))
    .select(
        "id",
        "page",
        "duration_sec",
        "session_id",
        F.row_number().over(w).alias("rn"),
    )
    .filter(F.col("rn") <= 3)
)
