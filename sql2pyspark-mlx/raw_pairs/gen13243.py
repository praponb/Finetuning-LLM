from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.year("delivered_date") == 2023)
    .select("id", "carrier")
    .unionByName(
        df_shipments
        .filter(F.year("delivered_date") == 2023)
        .select("id", "carrier")
    )
    .distinct()
)
