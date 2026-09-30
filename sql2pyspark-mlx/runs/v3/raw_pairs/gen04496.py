from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("opened_date", "id")

result = (
    df_stores
    .select(
        "id",
        "opened_date",
        "sqft",
        (F.col("sqft") - F.lag("sqft", 2).over(w)).alias("sqft_change"),
    )
)
