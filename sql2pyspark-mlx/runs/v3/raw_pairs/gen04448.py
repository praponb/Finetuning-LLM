from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("shipped_date", "id")

result = (
    df_shipments
    .filter(F.col("carrier").isNotNull())
    .select(
        "id",
        "carrier",
        "shipped_date",
        "cost",
        F.lead("cost", 1).over(w).alias("next_cost"),
    )
    .orderBy("carrier")
)
