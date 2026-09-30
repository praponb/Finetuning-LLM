from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("opened_date", "id")

result = (
    df_stores
    .filter(F.col("city").isin(["Tokyo", "Bangkok"]))
    .select(
        "id",
        "city",
        "opened_date",
        "sqft",
        F.lag("sqft", 1).over(w).alias("prev_sqft"),
    )
    .orderBy(F.col("opened_date").desc())
)
