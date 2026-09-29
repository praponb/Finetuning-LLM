from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("sqft", "id")
w2 = Window.partitionBy("city").orderBy("opened_date", "id")

result = (
    df_stores
    .select(
        "id",
        "region",
        "sqft",
        "city",
        "opened_date",
        F.row_number().over(w).alias("sqft_row"),
        F.lead("sqft", 1).over(w2).alias("next_sqft"),
    )
)
